use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;

use crate::app_state::AppState;
use crate::handlers::agent::schedules::types::{CreateScheduleRequest, PatchScheduleRequest};
use crate::schedule_app::{MisFirePolicy, OverlapPolicy, ScheduleRunConfig, ScheduleTrigger};
use bamboo_engine::model_config_helper::get_schedule_model_from_config;

pub(super) fn validate_schedule_name(name: &str) -> Result<String, HttpResponse> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("name is required")
        })));
    }
    Ok(trimmed.to_string())
}

fn validate_interval_seconds(interval_seconds: u64) -> Result<(), HttpResponse> {
    if interval_seconds == 0 {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("trigger.every_seconds must be > 0")
        })));
    }
    Ok(())
}

pub(super) fn validate_schedule_trigger(trigger: &ScheduleTrigger) -> Result<(), HttpResponse> {
    match trigger {
        ScheduleTrigger::Interval { every_seconds, .. } => {
            validate_interval_seconds(*every_seconds)
        }
        ScheduleTrigger::Once { at } => {
            // A one-shot whose instant already passed has no next occurrence,
            // so the store would fail computing the initial next run
            // (compute_initial_next_run_at) and surface a 500. Reject it here
            // with a 400 instead, mirroring the cron/timezone pre-checks.
            if *at <= Utc::now() {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value("trigger.at must be in the future")
                })));
            }
            Ok(())
        }
        ScheduleTrigger::Daily {
            hour,
            minute,
            second,
        } => validate_hms(*hour, *minute, *second),
        ScheduleTrigger::Weekly {
            weekdays,
            hour,
            minute,
            second,
        } => {
            if weekdays.is_empty() {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value("trigger.weekdays must not be empty")
                })));
            }
            validate_hms(*hour, *minute, *second)
        }
        ScheduleTrigger::Monthly {
            days,
            hour,
            minute,
            second,
        } => {
            if days.is_empty() {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value("trigger.days must not be empty")
                })));
            }
            if days.iter().any(|day| *day == 0 || *day > 31) {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value("trigger.days values must be between 1 and 31")
                })));
            }
            validate_hms(*hour, *minute, *second)
        }
        ScheduleTrigger::Cron { expr } => {
            let expr = expr.trim();
            if expr.is_empty() {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value("trigger.expr is required")
                })));
            }
            // Parse with the same `cron::Schedule` the trigger engine uses, so an
            // invalid expression fails here with a 400 instead of deep in the
            // store (compute_initial_next_run_at) where it surfaces as a 500.
            if expr.parse::<CronSchedule>().is_err() {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value(format!("trigger.expr is not a valid cron expression: {expr}"))
                })));
            }
            Ok(())
        }
    }
}

pub(super) fn validate_schedule_window(
    start_at: Option<DateTime<Utc>>,
    end_at: Option<DateTime<Utc>>,
) -> Result<(), HttpResponse> {
    if let (Some(start_at), Some(end_at)) = (start_at, end_at) {
        if start_at >= end_at {
            return Err(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value("start_at must be earlier than end_at")
            })));
        }
    }
    Ok(())
}

pub(super) fn validate_trigger_api_fields(
    trigger: Option<&ScheduleTrigger>,
    timezone: Option<&str>,
    start_at: Option<DateTime<Utc>>,
    end_at: Option<DateTime<Utc>>,
    misfire_policy: Option<MisFirePolicy>,
    overlap_policy: Option<OverlapPolicy>,
) -> Result<(), HttpResponse> {
    if let Some(trigger) = trigger {
        validate_schedule_trigger(trigger)?;
    }

    if let Some(timezone) = timezone.map(str::trim) {
        if timezone.is_empty() {
            return Err(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value("timezone must not be empty when provided")
            })));
        }
        // Same `chrono_tz::Tz` parse the trigger engine uses (parse_timezone);
        // reject a bogus zone with a 400 rather than a later 500.
        if timezone.parse::<Tz>().is_err() {
            return Err(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value(format!("timezone is not a valid IANA timezone: {timezone}"))
            })));
        }
    }

    validate_schedule_window(start_at, end_at)?;

    let _ = misfire_policy;
    let _ = overlap_policy;

    Ok(())
}

#[derive(Debug)]
pub(super) struct ResolvedCreateScheduleDefinition {
    pub definition: crate::schedule_app::store::ScheduleDefinitionChanges,
}

#[derive(Debug)]
pub(super) struct ResolvedPatchScheduleDefinition {
    pub definition: crate::schedule_app::store::ScheduleDefinitionChanges,
}

pub(super) fn resolve_create_schedule_definition(
    req: &CreateScheduleRequest,
) -> Result<ResolvedCreateScheduleDefinition, HttpResponse> {
    validate_trigger_api_fields(
        Some(&req.trigger),
        req.timezone.as_deref(),
        req.start_at,
        req.end_at,
        req.misfire_policy,
        req.overlap_policy,
    )?;

    Ok(ResolvedCreateScheduleDefinition {
        definition: crate::schedule_app::store::ScheduleDefinitionChanges {
            trigger: Some(req.trigger.clone()),
            timezone: req.timezone.clone(),
            start_at: req.start_at,
            end_at: req.end_at,
            misfire_policy: req.misfire_policy,
            overlap_policy: req.overlap_policy,
        },
    })
}

pub(super) fn resolve_patch_schedule_definition(
    req: &PatchScheduleRequest,
) -> Result<ResolvedPatchScheduleDefinition, HttpResponse> {
    validate_trigger_api_fields(
        req.trigger.as_ref(),
        req.timezone.as_deref(),
        req.start_at,
        req.end_at,
        req.misfire_policy,
        req.overlap_policy,
    )?;

    Ok(ResolvedPatchScheduleDefinition {
        definition: crate::schedule_app::store::ScheduleDefinitionChanges {
            trigger: req.trigger.clone(),
            timezone: req.timezone.clone(),
            start_at: req.start_at,
            end_at: req.end_at,
            misfire_policy: req.misfire_policy,
            overlap_policy: req.overlap_policy,
        },
    })
}

fn validate_hms(hour: u8, minute: u8, second: u8) -> Result<(), HttpResponse> {
    if hour > 23 {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("trigger.hour must be between 0 and 23")
        })));
    }
    if minute > 59 {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("trigger.minute must be between 0 and 59")
        })));
    }
    if second > 59 {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("trigger.second must be between 0 and 59")
        })));
    }
    Ok(())
}

pub(super) async fn validate_auto_execute_run_config(
    state: &web::Data<AppState>,
    run_config: &ScheduleRunConfig,
) -> Result<ScheduleRunConfig, HttpResponse> {
    let mut normalized = run_config.clone();
    if let Some(project_id) = run_config.project_id.as_ref() {
        match state.project_store.get(project_id) {
            Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {}
            Ok(_) => {
                return Err(HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "project_archived",
                        "message": "run_config.project_id must reference an active Project"
                    },
                    "project_id": project_id,
                })));
            }
            Err(bamboo_projects::ProjectStoreError::NotFound(_)) => {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value(
                        "run_config.project_id references a Project that does not exist"
                    ),
                    "project_id": project_id,
                })));
            }
            Err(error) => {
                return Err(HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": crate::error::error_value(format!(
                        "failed to validate run_config.project_id: {error}"
                    ))
                })));
            }
        }
    }
    let validated_workspace =
        match crate::project_context::validate_workspace_assignment_with_resolver(
            &state.project_store,
            run_config.project_id.as_ref(),
            run_config.workspace_path.as_deref(),
            &state.workspace_resolver,
        ) {
            Ok(workspace) => workspace,
            Err(crate::project_context::ProjectWorkspaceValidationError::Invalid {
                code,
                workspace,
                message,
            }) => {
                let mut response = if code.starts_with("project_path_") {
                    HttpResponse::Conflict()
                } else {
                    HttpResponse::BadRequest()
                };
                return Err(response.json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": code,
                        "message": message
                    },
                    "workspace": workspace,
                })));
            }
            Err(crate::project_context::ProjectWorkspaceValidationError::Conflict {
                workspace,
                owner_project_id,
                session_project_id,
            }) => {
                return Err(HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "project_workspace_conflict",
                        "message": "Workspace belongs to another Project"
                    },
                    "workspace": workspace,
                    "owner_project_id": owner_project_id,
                    "session_project_id": session_project_id,
                })));
            }
            Err(crate::project_context::ProjectWorkspaceValidationError::Store(error)) => {
                return Err(HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": crate::error::error_value(format!(
                        "failed to validate run_config.workspace_path: {error}"
                    ))
                })));
            }
        };
    // Keep omission durable so execution resolves the current Project path.
    // Persisting the validated fallback here would turn a Project default into
    // an explicit path and make later Project path CAS updates ineffective.
    normalized.workspace_path = run_config
        .workspace_path
        .as_deref()
        .map(str::trim)
        .filter(|workspace| !workspace.is_empty())
        .and(
            validated_workspace
                .as_deref()
                .map(bamboo_config::paths::path_to_display_string),
        );
    if !run_config.auto_execute {
        return Ok(normalized);
    }

    let has_task = run_config
        .task_message
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();

    if !has_task {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("run_config.task_message is required when auto_execute is true")
        })));
    }

    let has_explicit_model = run_config
        .model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    if has_explicit_model {
        return Ok(normalized);
    }

    let snapshot = state.config.read().await.clone();
    if let Err(error) = get_schedule_model_from_config(&snapshot) {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value(format!(
                "run_config.model not provided and no fast/default model configured for provider {}: {}",
                snapshot.provider, error
            ))
        })));
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule_app::ScheduleWeekday;

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
    fn validate_schedule_name_accepts_unicode() {
        let result = validate_schedule_name("任务计划 🎯");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "任务计划 🎯");
    }

    #[test]
    fn validate_interval_trigger_rejects_zero_every_seconds() {
        let result = validate_schedule_trigger(&ScheduleTrigger::Interval {
            every_seconds: 0,
            anchor_at: None,
        });
        assert!(result.is_err());
    }

    #[test]
    fn resolve_create_schedule_definition_accepts_interval_trigger() {
        let req = CreateScheduleRequest {
            name: "interval".to_string(),
            trigger: ScheduleTrigger::Interval {
                every_seconds: 120,
                anchor_at: None,
            },
            timezone: Some("Asia/Shanghai".to_string()),
            start_at: None,
            end_at: None,
            misfire_policy: None,
            overlap_policy: None,
            enabled: false,
            run_config: ScheduleRunConfig::default(),
        };
        let resolved = resolve_create_schedule_definition(&req).unwrap();
        assert!(matches!(
            resolved.definition.trigger,
            Some(ScheduleTrigger::Interval {
                every_seconds: 120,
                ..
            })
        ));
        assert_eq!(
            resolved.definition.timezone.as_deref(),
            Some("Asia/Shanghai")
        );
    }

    #[test]
    fn resolve_create_schedule_definition_accepts_daily_trigger() {
        let req = CreateScheduleRequest {
            name: "daily".to_string(),
            trigger: ScheduleTrigger::Daily {
                hour: 9,
                minute: 0,
                second: 0,
            },
            timezone: Some("Asia/Shanghai".to_string()),
            start_at: None,
            end_at: None,
            misfire_policy: None,
            overlap_policy: None,
            enabled: false,
            run_config: ScheduleRunConfig::default(),
        };
        let resolved = resolve_create_schedule_definition(&req).unwrap();
        assert!(matches!(
            resolved.definition.trigger,
            Some(ScheduleTrigger::Daily {
                hour: 9,
                minute: 0,
                second: 0
            })
        ));
        assert_eq!(
            resolved.definition.timezone.as_deref(),
            Some("Asia/Shanghai")
        );
    }

    #[test]
    fn resolve_patch_schedule_definition_accepts_optional_trigger() {
        let req = PatchScheduleRequest {
            name: None,
            enabled: Some(true),
            trigger: Some(ScheduleTrigger::Cron {
                expr: "0 0 * * * *".to_string(),
            }),
            timezone: Some("UTC".to_string()),
            start_at: None,
            end_at: None,
            misfire_policy: None,
            overlap_policy: None,
            run_config: None,
        };
        let resolved = resolve_patch_schedule_definition(&req).unwrap();
        assert!(matches!(
            resolved.definition.trigger,
            Some(ScheduleTrigger::Cron { .. })
        ));
        assert_eq!(resolved.definition.timezone.as_deref(), Some("UTC"));
    }

    #[test]
    fn validate_schedule_trigger_accepts_future_once() {
        validate_schedule_trigger(&ScheduleTrigger::Once {
            at: Utc::now() + chrono::Duration::seconds(600),
        })
        .expect("a one-shot trigger in the future should pass validation");
    }

    #[test]
    fn validate_schedule_trigger_rejects_past_once() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Once {
            at: Utc::now() - chrono::Duration::seconds(600),
        })
        .expect_err("a one-shot trigger in the past must be a 400, not a later 500");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_window_rejects_inverted_range() {
        let start_at = DateTime::parse_from_rfc3339("2026-04-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end_at = DateTime::parse_from_rfc3339("2026-04-04T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let response = validate_schedule_window(Some(start_at), Some(end_at))
            .expect_err("window should reject inverted range");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_rejects_empty_weekdays() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Weekly {
            weekdays: vec![],
            hour: 9,
            minute: 0,
            second: 0,
        })
        .expect_err("weekly trigger should require weekdays");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_accepts_weekly_with_days() {
        validate_schedule_trigger(&ScheduleTrigger::Weekly {
            weekdays: vec![ScheduleWeekday::Mon],
            hour: 9,
            minute: 0,
            second: 0,
        })
        .expect("weekly trigger should accept weekdays");
    }

    #[test]
    fn validate_schedule_trigger_rejects_monthly_day_out_of_range() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Monthly {
            days: vec![0, 32],
            hour: 9,
            minute: 0,
            second: 0,
        })
        .expect_err("monthly trigger should reject out-of-range days");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_rejects_invalid_hour() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Daily {
            hour: 24,
            minute: 0,
            second: 0,
        })
        .expect_err("daily trigger should reject invalid hour");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_rejects_invalid_minute() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Weekly {
            weekdays: vec![ScheduleWeekday::Tue],
            hour: 10,
            minute: 60,
            second: 0,
        })
        .expect_err("weekly trigger should reject invalid minute");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_rejects_invalid_second() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Monthly {
            days: vec![1],
            hour: 10,
            minute: 30,
            second: 60,
        })
        .expect_err("monthly trigger should reject invalid second");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_rejects_blank_cron_expr() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Cron {
            expr: "   ".to_string(),
        })
        .expect_err("cron trigger should reject blank expr");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_trigger_api_fields_rejects_blank_timezone() {
        let response = validate_trigger_api_fields(
            Some(&ScheduleTrigger::Daily {
                hour: 9,
                minute: 0,
                second: 0,
            }),
            Some("   "),
            None,
            None,
            None,
            None,
        )
        .expect_err("blank timezone should be rejected");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_rejects_invalid_cron_expr() {
        let response = validate_schedule_trigger(&ScheduleTrigger::Cron {
            expr: "not a cron expression".to_string(),
        })
        .expect_err("a non-empty but unparseable cron expr must be a 400, not a later 500");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_schedule_trigger_accepts_valid_cron_expr() {
        // 6-field cron (sec min hour day-of-month month day-of-week): noon daily.
        validate_schedule_trigger(&ScheduleTrigger::Cron {
            expr: "0 0 12 * * *".to_string(),
        })
        .expect("a valid cron expr should pass validation");
    }

    #[test]
    fn validate_trigger_api_fields_rejects_invalid_timezone() {
        let response = validate_trigger_api_fields(
            Some(&ScheduleTrigger::Daily {
                hour: 9,
                minute: 0,
                second: 0,
            }),
            Some("Mars/Phobos"),
            None,
            None,
            None,
            None,
        )
        .expect_err("a bogus IANA timezone must be a 400, not a later 500");
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_trigger_api_fields_accepts_valid_timezone() {
        validate_trigger_api_fields(
            Some(&ScheduleTrigger::Daily {
                hour: 9,
                minute: 0,
                second: 0,
            }),
            Some("America/New_York"),
            None,
            None,
            None,
            None,
        )
        .expect("a valid trigger + IANA timezone should pass validation");
    }
}
